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
    let writeMode: RepoWriteMode
    let encryptionContext: RepoEncryptionContext?

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
        writeMode: RepoWriteMode,
        encryptionContext: RepoEncryptionContext? = nil
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
        self.writeMode = writeMode
        self.encryptionContext = encryptionContext
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
            writeMode: writeMode,
            encryptionContext: encryptionContext
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
    let uploadFileSize: Int64
    let storageCodec: Int
    let storedFileSize: Int64?
    let encryptionKeyID: String?
    let shotDate: Date?

    init(
        local: LocalPhotoResource,
        tempFileURL: URL,
        contentHash: Data,
        fileSize: Int64,
        uploadFileSize: Int64? = nil,
        storageCodec: Int = RemoteManifestResource.plaintextStorageCodec,
        storedFileSize: Int64? = nil,
        encryptionKeyID: String? = nil,
        shotDate: Date?
    ) {
        self.local = local
        self.tempFileURL = tempFileURL
        self.contentHash = contentHash
        self.fileSize = fileSize
        self.uploadFileSize = uploadFileSize ?? fileSize
        self.storageCodec = storageCodec
        self.storedFileSize = storedFileSize
        self.encryptionKeyID = encryptionKeyID
        self.shotDate = shotDate
    }
}

struct ResourceUploadResult: Sendable {
    let status: BackupItemStatus
    let reason: String?
    let fileName: String?

    init(status: BackupItemStatus, reason: String?, fileName: String? = nil) {
        self.status = status
        self.reason = reason
        self.fileName = fileName
    }
}
