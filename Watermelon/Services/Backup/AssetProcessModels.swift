import Foundation
import Photos

struct AssetProcessContext {
    let workerID: Int
    let asset: PHAsset
    let selectedResources: [BackupSelectedResource]
    let cachedLocalHash: LocalAssetHashCache?
    let iCloudPhotoBackupMode: ICloudPhotoBackupMode
    let monthStore: MonthManifestStore
    let profile: ServerProfileRecord
    let assetPosition: Int
    let totalAssets: Int
    // Live Lite write lease, or nil under V1. Gates remote data writes in the upload path.
    let liteSession: LiteWriteSession?

    init(
        workerID: Int,
        asset: PHAsset,
        selectedResources: [BackupSelectedResource],
        cachedLocalHash: LocalAssetHashCache?,
        iCloudPhotoBackupMode: ICloudPhotoBackupMode,
        monthStore: MonthManifestStore,
        profile: ServerProfileRecord,
        assetPosition: Int,
        totalAssets: Int,
        liteSession: LiteWriteSession? = nil
    ) {
        self.workerID = workerID
        self.asset = asset
        self.selectedResources = selectedResources
        self.cachedLocalHash = cachedLocalHash
        self.iCloudPhotoBackupMode = iCloudPhotoBackupMode
        self.monthStore = monthStore
        self.profile = profile
        self.assetPosition = assetPosition
        self.totalAssets = totalAssets
        self.liteSession = liteSession
    }

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
            totalAssets: totalAssets,
            liteSession: liteSession
        )
    }
}

struct AssetProcessResult: Sendable {
    let status: BackupItemStatus
    let reason: String?
    let displayName: String
    let assetFingerprint: Data?
    let timing: AssetProcessTiming
    let totalFileSizeBytes: Int64
    let uploadedFileSizeBytes: Int64
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
