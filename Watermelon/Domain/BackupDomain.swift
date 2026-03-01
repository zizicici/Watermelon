import Foundation
import Photos

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
    let resourceSummary: String?
    let updatedAt: Date
}

struct BackupTransferState {
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

    var clampedResourceFraction: Float {
        min(max(resourceFraction, 0), 1)
    }

    var assetFraction: Float {
        guard totalResources > 0 else { return 0 }
        let completedResources = Float(max(resourcePosition - 1, 0))
        return min(max((completedResources + clampedResourceFraction) / Float(totalResources), 0), 1)
    }

    var overallFraction: Float {
        guard totalAssets > 0 else { return 0 }
        let completedAssets = Float(max(assetPosition - 1, 0))
        return min(max((completedAssets + assetFraction) / Float(totalAssets), 0), 1)
    }
}

struct LocalPhotoResource {
    let asset: PHAsset
    let resource: PHAssetResource
    let assetLocalIdentifier: String
    let resourceLocalIdentifier: String
    let resourceRole: Int
    let resourceSlot: Int
    let resourceType: String
    let resourceTypeCode: Int
    let uti: String?
    let originalFilename: String
    let fileSize: Int64
    let resourceModificationDate: Date?
}

struct BackupProgress {
    let succeeded: Int
    let failed: Int
    let skipped: Int
    let total: Int
    let message: String
    let itemEvent: BackupItemEvent?
    let transferState: BackupTransferState?

    var processed: Int {
        succeeded + failed + skipped
    }

    var fraction: Float {
        guard total > 0 else { return 0 }
        return Float(processed) / Float(total)
    }
}

enum BackupError: LocalizedError {
    case missingServerProfile
    case missingCredentials
    case photoPermissionDenied
    case smbUnavailable
    case restoreNoSelection

    var errorDescription: String? {
        switch self {
        case .missingServerProfile:
            return "No server profile configured."
        case .missingCredentials:
            return "Missing server credentials."
        case .photoPermissionDenied:
            return "Photo library permission denied."
        case .smbUnavailable:
            return "SMB support is unavailable in this build."
        case .restoreNoSelection:
            return "No items selected for restore."
        }
    }
}
