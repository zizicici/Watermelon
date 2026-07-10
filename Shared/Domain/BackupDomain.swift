import Foundation
#if os(iOS)
import Photos
#endif

enum ExecutionLogLevel: String, CaseIterable, Sendable {
    case debug
    case info
    case warning
    case error
}

struct BackupExecutionResult: Sendable {
    let total: Int
    let succeeded: Int
    let failed: Int
    let skipped: Int
    let paused: Bool
}

enum BackupItemStatus: String, Codable, Sendable {
    case success
    case failed
    case skipped
}

enum BackupTransferKind: Hashable, Sendable {
    case upload
    case download
}

struct BackupItemEvent: Sendable {
    let assetLocalIdentifier: String
    let assetFingerprint: Data?
    let month: LibraryMonthKey
    let displayName: String
    let resourceDate: Date?
    let status: BackupItemStatus
    let reason: String?
    let updatedAt: Date
}

struct BackupTransferState: Sendable {
    let kind: BackupTransferKind
    let workerID: Int
    let assetLocalIdentifier: String
    let assetDisplayName: String
    let resourceDate: Date?
    let assetPosition: Int
    let totalAssets: Int
    let resourceDisplayName: String
    let resourcePosition: Int
    let totalResources: Int
    let resourceFraction: Float
    let resourceBytesTransferred: Int64?
    let resourceTotalBytes: Int64?
    let countsTowardTransferSpeed: Bool
    let stageDescription: String
}

#if os(iOS)
struct LocalPhotoResource {
    let asset: PHAsset
    let resource: PHAssetResource
    let assetLocalIdentifier: String
    let resourceLocalIdentifier: String
    let preferredRemoteFileName: String
    let resourceRole: Int
    let resourceSlot: Int
    let resourceType: String
    let resourceTypeCode: Int
    let uti: String?
    let originalFilename: String
    let fileSize: Int64
    let resourceModificationDate: Date?
}
#endif

struct BackupProgress: Sendable {
    let succeeded: Int
    let failed: Int
    let skipped: Int
    let total: Int
    let message: String
    let logMessage: String?
    let logLevel: ExecutionLogLevel
    let itemEvent: BackupItemEvent?
    let transferState: BackupTransferState?

    var effectiveLogMessage: String { logMessage ?? message }
}

enum BackupError: LocalizedError, Equatable {
    case photoPermissionDenied
    case restoreNoSelection
    case resourceEncryptionNotConfirmed

    var errorDescription: String? {
        switch self {
        case .photoPermissionDenied:
            return String(localized: "backup.error.photoPermissionDenied")
        case .restoreNoSelection:
            return String(localized: "backup.error.restoreNoSelection")
        case .resourceEncryptionNotConfirmed:
            return String(
                localized: "backup.error.resourceEncryptionNotConfirmed",
                defaultValue: "This repository is encrypted. Save or import the recovery key before running backup."
            )
        }
    }
}
