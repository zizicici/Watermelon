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
}

struct AssetProcessResult: Sendable {
    let status: BackupItemStatus
    let reason: String?
    let displayName: String
    let resourceSummary: String
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
