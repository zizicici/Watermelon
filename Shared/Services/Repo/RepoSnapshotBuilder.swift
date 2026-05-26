import Foundation

/// Builds snapshots only when state exactly equals fold(commits in covered).
enum RepoSnapshotBuilder {
    /// Sorting is identity-stable so the same state always yields the same SHA.
    static func build(
        header: SnapshotHeader,
        state: RepoMonthState
    ) -> (
        assets: [SnapshotAssetRow],
        resources: [SnapshotResourceRow],
        assetResources: [SnapshotAssetResourceRow],
        deletedKeys: [SnapshotDeletedKeyRow]
    ) {
        let assets = state.assets.values.map { row in
            SnapshotAssetRow(
                assetFingerprint: row.assetFingerprint,
                creationDateMs: row.creationDateMs,
                backedUpAtMs: row.backedUpAtMs,
                resourceCount: row.resourceCount,
                totalFileSizeBytes: row.totalFileSizeBytes,
                stamp: row.stamp
            )
        }
        let resources = state.resources.values.map { row in
            SnapshotResourceRow(
                physicalRemotePath: row.physicalRemotePath,
                contentHash: row.contentHash,
                fileSize: row.fileSize,
                resourceType: row.resourceType,
                creationDateMs: row.creationDateMs,
                backedUpAtMs: row.backedUpAtMs,
                crypto: row.crypto,
                stamp: row.stamp
            )
        }
        let assetResources = state.assetResources.values.map { row in
            SnapshotAssetResourceRow(
                assetFingerprint: row.assetFingerprint,
                role: row.role,
                slot: row.slot,
                resourceHash: row.resourceHash,
                logicalName: row.logicalName
            )
        }
        let deletedKeys = state.deletedAssetStamps.map { fp, stamp in
            SnapshotDeletedKeyRow(
                keyType: .asset,
                keyValue: fp.hexString,
                stamp: stamp
            )
        }
        return (assets: assets, resources: resources, assetResources: assetResources, deletedKeys: deletedKeys)
    }
}
