import Foundation
import Photos

struct AssetProcessContext {
    let workerID: Int
    let asset: PHAsset
    let selectedResources: [BackupSelectedResource]
    let cachedLocalHash: LocalAssetHashCache?
    let iCloudPhotoBackupMode: ICloudPhotoBackupMode
    let monthStore: any BackupMonthStore
    let profile: ServerProfileRecord
    let assetPosition: Int
    let totalAssets: Int

    func withRefreshedAsset(
        _ asset: PHAsset,
        selectedResources: [BackupSelectedResource]
    ) -> AssetProcessContext {
        AssetProcessContext(
            workerID: workerID,
            asset: asset,
            selectedResources: selectedResources,
            cachedLocalHash: cachedLocalHash,
            iCloudPhotoBackupMode: iCloudPhotoBackupMode,
            monthStore: monthStore,
            profile: profile,
            assetPosition: assetPosition,
            totalAssets: totalAssets
        )
    }
}

struct AssetProcessResult: Sendable {
    let status: BackupItemStatus
    let reason: String?
    let displayName: String
    let assetFingerprint: AssetFingerprint?
    let timing: AssetProcessTiming
    let totalFileSizeBytes: Int64
    let uploadedFileSizeBytes: Int64
    /// True iff this result added a pending V2 row to `V2MonthIndexes` AND queued a hash-index
    /// intent that will need post-batch-commit reconciliation. The cached-durable short-circuit
    /// (`reason == "asset_exists_cached"`) writes its hash-index row inline and never reaches
    /// `finalizeRowWritingAsset`, so it sets this to `false`. The provisional progress buffer
    /// in `ParallelBackupProgressAggregator` only records results where this is `true`.
    var wroteProvisionalV2Row: Bool = false
    /// Subset fingerprints that this asset's `upsertAsset` tombstoned in-memory because their
    /// resource link sets are strict subsets of this asset's superset. The aggregator pairs these
    /// with this asset's carrier fingerprint so a later `markBatchDurable(adds:F_carrier)` cascade
    /// clears the subset entries — including the case where the in-batch tombstone op never
    /// reaches a durable commit (e.g. it lands in a later chunk that fails on connection loss).
    var tombstonedSubsetFingerprints: Set<AssetFingerprint> = []
}

struct AssetProcessTiming: Sendable {
    var exportHashSeconds: TimeInterval = 0
    var collisionCheckSeconds: TimeInterval = 0
    var uploadBodySeconds: TimeInterval = 0
    var setModificationDateSeconds: TimeInterval = 0
    var databaseSeconds: TimeInterval = 0
}

struct PreparedResource: Sendable {
    let local: LocalPhotoResource
    let tempFileURL: URL
    let contentHash: Data
    let fileSize: Int64
    let shotDate: Date?
}

struct ResourceUploadResult: Sendable {
    let status: BackupItemStatus
    let reason: String?
}
