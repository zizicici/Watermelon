import Foundation

/// Pure-function snapshot builder. The contract: `snapshot.covered = [lo, hi]`
/// truly means `state == fold(commit ops in [lo, hi])`. The builder does NOT
/// filter assets/links/resources based on actionability — orphan-resource
/// handling, filename presence, etc. all belong to the session view layer
/// (`V2MonthSession.findResourceByHash` etc), NEVER to the snapshot writer.
///
/// Earlier rounds collapsed this distinction: writeSnapshot ran an
/// orphan-link suppression pass, breaking the covered-range invariant
/// (snapshot dropped state but still claimed to cover the commits that
/// produced it). Materializer would then skip replaying those commits, losing
/// historical evidence permanently.
enum RepoSnapshotBuilder {
    /// Build the SnapshotFile bytes from a materialized state + header.
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
                crypto: row.crypto
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
        let deletedKeys = state.deletedAssetFingerprints.map { fp in
            SnapshotDeletedKeyRow(
                keyType: .asset,
                keyValue: fp.hexString,
                stamp: state.deletedAssetStamps[fp]
            )
        }
        return (assets: assets, resources: resources, assetResources: assetResources, deletedKeys: deletedKeys)
    }
}
