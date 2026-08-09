import Foundation
import Photos

enum AssetUploadPass: Sendable {
    case localResources
    case iCloudResources
}

struct AssetProcessContext {
    let workerID: Int
    let asset: PHAsset
    let selectedResources: [BackupSelectedResource]
    let cachedLocalHash: LocalAssetHashCache?
    let iCloudPhotoBackupMode: ICloudPhotoBackupMode
    let pass: AssetUploadPass
    let monthStore: MonthManifestStore
    let profile: ServerProfileRecord
    let assetPosition: Int
    let totalAssets: Int
    let writeMode: RepoWriteMode

    var allowsNetworkExport: Bool {
        switch pass {
        case .localResources:
            return false
        case .iCloudResources:
            return iCloudPhotoBackupMode.allowsNetworkAccess
        }
    }

    // Only the offline first pass may defer an asset to iCloud.
    var defersNetworkResources: Bool {
        pass == .localResources && iCloudPhotoBackupMode.allowsNetworkAccess
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
            pass: pass,
            monthStore: monthStore,
            profile: profile,
            assetPosition: assetPosition,
            totalAssets: totalAssets,
            writeMode: writeMode
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
