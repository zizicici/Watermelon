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

enum BackupItemStatus: String, Codable {
    case success
    case failed
    case skipped
}

struct BackupItemEvent {
    let assetLocalIdentifier: String
    let assetFingerprint: Data?
    let displayName: String
    let resourceDate: Date?
    let status: BackupItemStatus
    let reason: String?
    let updatedAt: Date
}

struct BackupTransferState {
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

struct BackupProgress {
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

enum BackupError: LocalizedError {
    case photoPermissionDenied
    case restoreNoSelection

    var errorDescription: String? {
        switch self {
        case .photoPermissionDenied:
            return String(localized: "backup.error.photoPermissionDenied")
        case .restoreNoSelection:
            return String(localized: "backup.error.restoreNoSelection")
        }
    }
}
