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
        for resource in resources where resource.contentHash.count == 32 {
            resourcesByHash[resource.contentHash] = resource
        }
        let linksByAssetFP: [AssetFingerprint: [RemoteAssetResourceLink]] = Dictionary(grouping: links, by: { $0.assetFingerprint })

        var migrable: [V1MigrableAsset] = []
        migrable.reserveCapacity(assets.count)
        var skippedFailures: [String] = []
        for asset in assets {
            let assetLinks = linksByAssetFP[asset.assetFingerprint] ?? []
            if assetLinks.isEmpty {
                skippedFailures.append("asset \(asset.assetFingerprint) has no resource links")
                continue
            }
            var resourcesForOp: [CommitResourceEntry] = []
            resourcesForOp.reserveCapacity(assetLinks.count)
            var missingResourceHash: Data?
            var invalidResourceHash: Data?
            for link in assetLinks {
                guard link.resourceHash.count == 32 else {
                    invalidResourceHash = link.resourceHash
                    break
                }
                guard let res = resourcesByHash[link.resourceHash] else {
                    missingResourceHash = link.resourceHash
                    break
                }
                guard res.contentHash.count == 32 else {
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
                skippedFailures.append("asset \(asset.assetFingerprint) references invalid resource hash length \(invalidResourceHash.count)")
                continue
            }
            if let missingResourceHash {
                skippedFailures.append("asset \(asset.assetFingerprint) references missing resource \(missingResourceHash.hexString)")
                continue
            }
            if let wireFailure = wireSchemaFailure(asset: asset, resources: resourcesForOp) {
                skippedFailures.append(wireFailure)
                continue
            }
            migrable.append(V1MigrableAsset(asset: asset, resources: resourcesForOp))
        }
        return V1MonthMigrationPlan(migrable: migrable, skippedFailures: skippedFailures)
    }

    /// V1 columns are typed but unvalidated; a corrupt legacy row can hold a scalar/path/name the V2
    /// commit/snapshot decoders reject (negative role/slot/fileSize/resourceType/timestamp, `..` path,
    /// unsafe logical name) or a path the materializer's replay/body check refuses (a no-`..` but
    /// out-of-month / wrong-component-count path). The migration writer does not re-validate on encode,
    /// so admitting such a row would publish a commit/snapshot the materializer later refuses — folding
    /// the whole month corrupt after it is journaled `.imported` and the manifest is quarantined (no
    /// retry). Gate the V1→V2 admission on the readers' own `RepoWireValidator` plus the materializer's
    /// `resourcePathBelongsToMonth` so the offending asset is skipped/quarantined instead. Honest V1
    /// values satisfy every rule (honest paths are `YYYY/MM/leaf` for their month), so this only fires
    /// on corrupt legacy bytes.
    private static func wireSchemaFailure(asset: RemoteManifestAsset, resources: [CommitResourceEntry]) -> String? {
        do {
            _ = try RepoWireValidator.validateNonNegativeInt64(asset.backedUpAtMs, field: "backedUpAtMs")
            if let creationDateMs = asset.creationDateMs {
                _ = try RepoWireValidator.validateNonNegativeInt64(creationDateMs, field: "creationDateMs")
            }
            for resource in resources {
                _ = try RepoWireValidator.validateNonNegativeInt(resource.role, field: "role")
                _ = try RepoWireValidator.validateNonNegativeInt(resource.slot, field: "slot")
                _ = try RepoWireValidator.validateNonNegativeInt64(resource.fileSize, field: "fileSize")
                _ = try RepoWireValidator.validateNonNegativeInt(resource.resourceType, field: "resourceType")
                _ = try RepoWireValidator.validateRelativePath(resource.physicalRemotePath)
                _ = try RepoWireValidator.validateLogicalName(resource.logicalName, field: "logicalName")
            }
        } catch let error as WireValidationError {
            return "asset \(asset.assetFingerprint) violates V2 wire schema: \(String(describing: error))"
        } catch {
            return "asset \(asset.assetFingerprint) violates V2 wire schema"
        }
        // `resourcePathBelongsToMonth` is a materializer replay/body rule (SnapshotTrustPolicy, outside
        // RepoWireValidator); the asset's month is the directory month the commit is written under.
        let month = LibraryMonthKey(year: asset.year, month: asset.month)
        for resource in resources where !SnapshotTrustPolicy.resourcePathBelongsToMonth(resource.physicalRemotePath, month: month) {
            return "asset \(asset.assetFingerprint) resource path \(resource.physicalRemotePath) is not within month \(month.text)"
        }
        return nil
    }
}
