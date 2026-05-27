import Foundation

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
    // Ephemeral per-run UI string. Never persisted to remote storage.
    let assetLocalIdentifier: String
    let assetFingerprint: AssetFingerprint?
    let displayName: String
    let resourceDate: Date?
    let status: BackupItemStatus
    let reason: String?
    let updatedAt: Date
}

struct BackupTransferState {
    let workerID: Int
    // Ephemeral per-run UI string. Never persisted to remote storage.
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
