import Foundation

/// Adapter/export layer that composes the focused month authorities into the flat
/// `(resources, assets, links)` tuple `BackupMonthStore.unsortedSnapshot` and the committed-view
/// publish path consume. Stateless: resources come from the live presence working set, assets and
/// links from the committed-row authority — matching the pre-split `V2MonthIndexes` export exactly.
enum SnapshotProjection {
    static func unsortedSnapshot(
        committed: RepoMonthCommittedState,
        presence: MonthPresenceProjection
    ) -> (resources: [RemoteManifestResource], assets: [RemoteManifestAsset], links: [RemoteAssetResourceLink]) {
        (presence.allResources(), committed.allAssets(), committed.allLinks())
    }

    // MARK: - Differential normalization

    /// Order-independent, path-agnostic view of a month snapshot for V1/V2 differential comparison.
    /// Resources collapse to content-hash facts (V1 keys by logical name, V2 by physical path, so a
    /// path-keyed compare would diverge even when the content is equivalent); assets and links keep
    /// their identity keys.
    struct Normalized: Equatable {
        let resourceFactsByHash: [Data: ResourceFacts]
        let assetFingerprints: Set<AssetFingerprint>
        let links: Set<NormalizedLink>
    }

    struct ResourceFacts: Equatable {
        let fileSize: Int64
        let resourceType: Int
    }

    struct NormalizedLink: Hashable {
        let assetFingerprint: AssetFingerprint
        let role: Int
        let slot: Int
        let resourceHash: Data
    }

    static func normalize(
        _ snapshot: (resources: [RemoteManifestResource], assets: [RemoteManifestAsset], links: [RemoteAssetResourceLink])
    ) -> Normalized {
        var resourceFactsByHash: [Data: ResourceFacts] = [:]
        for resource in snapshot.resources {
            resourceFactsByHash[resource.contentHash] = ResourceFacts(
                fileSize: resource.fileSize,
                resourceType: resource.resourceType
            )
        }
        let assetFingerprints = Set(snapshot.assets.map(\.assetFingerprint))
        let links = Set(snapshot.links.map {
            NormalizedLink(
                assetFingerprint: $0.assetFingerprint,
                role: $0.role,
                slot: $0.slot,
                resourceHash: $0.resourceHash
            )
        })
        return Normalized(
            resourceFactsByHash: resourceFactsByHash,
            assetFingerprints: assetFingerprints,
            links: links
        )
    }

    static func normalize(
        _ store: any BackupMonthStore
    ) -> Normalized {
        normalize(store.unsortedSnapshot())
    }
}
