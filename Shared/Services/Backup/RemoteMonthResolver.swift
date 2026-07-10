import Foundation

/// Single source of truth for resolving a remote month's raw manifest (resources/assets/links) into the
/// displayable `RemoteMonthResolved` intermediate. Applies the partial-flush drop rule: an asset counts
/// only when at least one of its links points at a resource that is actually present; bytes are summed
/// over the deduped reachable resource hashes (not `asset.totalFileSizeBytes`).
enum RemoteMonthResolver {
    static func resolve(
        month: LibraryMonthKey,
        resources: [RemoteManifestResource],
        assets: [RemoteManifestAsset],
        links: [RemoteAssetResourceLink]
    ) -> RemoteMonthResolved {
        let empty = RemoteMonthResolved(
            month: month, assetCount: 0, photoCount: 0, videoCount: 0, totalSizeBytes: 0, fingerprints: []
        )
        guard !assets.isEmpty else { return empty }

        let resourceLookup = RemoteResourceLookup(resources)
        var resourceSizeByFileName: [String: Int64] = [:]
        resourceSizeByFileName.reserveCapacity(resources.count)
        for resource in resources {
            resourceSizeByFileName[resource.fileName] = max(resource.storedFileSize ?? resource.fileSize, 0)
        }

        // Per-asset: collect link roles and the dedup'd set of resolvable resource hashes. A link counts
        // only if its resource is present, so an asset whose resources haven't flushed yet is dropped.
        var rolesByAssetID: [String: [Int]] = [:]
        var resolvableFileNamesByAssetID: [String: Set<String>] = [:]
        rolesByAssetID.reserveCapacity(assets.count)
        resolvableFileNamesByAssetID.reserveCapacity(assets.count)
        for link in links {
            guard let resource = resourceLookup.resource(for: link) else { continue }
            rolesByAssetID[link.assetID, default: []].append(link.role)
            resolvableFileNamesByAssetID[link.assetID, default: []].insert(resource.fileName)
        }

        var fingerprints = Set<Data>()
        fingerprints.reserveCapacity(assets.count)
        var assetCount = 0
        var photoCount = 0
        var videoCount = 0
        var reachableFileNames = Set<String>()
        for asset in assets {
            let roles = rolesByAssetID[asset.id] ?? []
            // A record with no resolvable link, or only a config-only (adjustment sidecar) one, is not a real
            // backup: drop it so it neither inflates remote counts nor masks a local upload need. Mirrors the
            // manifest cleanup rule (cleanupMissingResources) that prunes such assets on the backup side.
            guard ResourceRole.containsRealMedia(roles) else { continue }
            fingerprints.insert(asset.assetFingerprint)
            assetCount += 1
            if let fileNames = resolvableFileNamesByAssetID[asset.id] {
                reachableFileNames.formUnion(fileNames)
            }
            // livePhoto folds into photoCount (two-bucket taxonomy); only a non-Live video counts as video.
            let (_, isVideo) = ResourceRole.classify(roles: roles)
            if isVideo { videoCount += 1 } else { photoCount += 1 }
        }
        guard assetCount > 0 else { return empty }

        let totalSize = reachableFileNames.reduce(Int64(0)) { $0 + (resourceSizeByFileName[$1] ?? 0) }
        return RemoteMonthResolved(
            month: month,
            assetCount: assetCount,
            photoCount: photoCount,
            videoCount: videoCount,
            totalSizeBytes: totalSize,
            fingerprints: fingerprints
        )
    }

    /// Resolve is pure per-month; fan the CPU-bound resolves across cores for a full/large snapshot, then
    /// the caller consumes results single-threaded. Small batches skip the concurrency overhead. Runs on the
    /// caller's thread — invoke off the main thread (HomeRemoteIndexEngine.apply runs on the worker queue).
    static func resolveMany(_ deltas: [RemoteLibraryMonthDelta]) -> [RemoteMonthResolved] {
        guard deltas.count >= 8 else {
            return deltas.map { resolve(month: $0.month, resources: $0.resources, assets: $0.assets, links: $0.assetResourceLinks) }
        }
        return [RemoteMonthResolved](unsafeUninitializedCapacity: deltas.count) { buffer, count in
            DispatchQueue.concurrentPerform(iterations: deltas.count) { i in
                let d = deltas[i]
                (buffer.baseAddress! + i).initialize(
                    to: resolve(month: d.month, resources: d.resources, assets: d.assets, links: d.assetResourceLinks)
                )
            }
            count = deltas.count
        }
    }
}
