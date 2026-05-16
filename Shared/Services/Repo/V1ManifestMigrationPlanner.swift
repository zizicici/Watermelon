import Foundation

// `nonisolated` keeps the planner and its value types isolation-neutral — WatermelonMac uses default-MainActor isolation, otherwise actor callers cross actors.
nonisolated struct V1MigrableAsset: Sendable {
    let asset: RemoteManifestAsset
    let resources: [CommitResourceEntry]
}

nonisolated struct V1MonthMigrationPlan: Sendable {
    let migrable: [V1MigrableAsset]
    /// Human-readable reasons written verbatim into `partial-migration-marker.json` on the remote;
    /// changing any string is a remote-schema break for tooling that reads existing markers.
    let skippedFailures: [String]
}

nonisolated enum V1ManifestMigrationPlanner {
    static func plan(
        assets: [RemoteManifestAsset],
        resources: [RemoteManifestResource],
        links: [RemoteAssetResourceLink]
    ) -> V1MonthMigrationPlan {
        var resourcesByHash: [Data: RemoteManifestResource] = [:]
        resourcesByHash.reserveCapacity(resources.count)
        for resource in resources where isValidV2Hash(resource.contentHash) {
            resourcesByHash[resource.contentHash] = resource
        }
        let linksByAssetFP: [Data: [RemoteAssetResourceLink]] = Dictionary(grouping: links, by: { $0.assetFingerprint })

        var migrable: [V1MigrableAsset] = []
        migrable.reserveCapacity(assets.count)
        var skippedFailures: [String] = []
        for asset in assets {
            guard isValidV2Hash(asset.assetFingerprint) else {
                skippedFailures.append("asset has invalid fingerprint length \(asset.assetFingerprint.count)")
                continue
            }
            let assetLinks = linksByAssetFP[asset.assetFingerprint] ?? []
            if assetLinks.isEmpty {
                skippedFailures.append("asset \(asset.assetFingerprint.hexString) has no resource links")
                continue
            }
            var resourcesForOp: [CommitResourceEntry] = []
            resourcesForOp.reserveCapacity(assetLinks.count)
            var missingResourceHash: Data?
            var invalidResourceHash: Data?
            for link in assetLinks {
                guard isValidV2Hash(link.resourceHash) else {
                    invalidResourceHash = link.resourceHash
                    break
                }
                guard let res = resourcesByHash[link.resourceHash] else {
                    missingResourceHash = link.resourceHash
                    break
                }
                guard isValidV2Hash(res.contentHash) else {
                    invalidResourceHash = res.contentHash
                    break
                }
                resourcesForOp.append(CommitResourceEntry(
                    physicalRemotePath: res.physicalRemotePath,
                    logicalName: link.logicalName.isEmpty ? res.logicalName : link.logicalName,
                    contentHash: res.contentHash,
                    fileSize: res.fileSize,
                    resourceType: res.resourceType,
                    role: link.role,
                    slot: link.slot,
                    crypto: res.crypto
                ))
            }
            if let invalidResourceHash {
                skippedFailures.append("asset \(asset.assetFingerprint.hexString) references invalid resource hash length \(invalidResourceHash.count)")
                continue
            }
            if let missingResourceHash {
                skippedFailures.append("asset \(asset.assetFingerprint.hexString) references missing resource \(missingResourceHash.hexString)")
                continue
            }
            migrable.append(V1MigrableAsset(asset: asset, resources: resourcesForOp))
        }
        return V1MonthMigrationPlan(migrable: migrable, skippedFailures: skippedFailures)
    }

    private static func isValidV2Hash(_ hash: Data) -> Bool {
        hash.count == 32
    }
}
