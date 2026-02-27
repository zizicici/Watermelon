import Foundation
import Photos

enum BackupItemStatus: String, Codable {
    case success
    case failed
    case skipped
}

struct BackupItemEvent {
    let assetLocalIdentifier: String
    let assetFingerprint: Data?
    let displayName: String
    let status: BackupItemStatus
    let reason: String?
    let resourceSummary: String?
    let updatedAt: Date
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

struct PlannedBackupItem {
    let localResource: LocalPhotoResource
    let fingerprint: String
    let remoteRelativePath: String
}

struct BackupProgress {
    let succeeded: Int
    let failed: Int
    let skipped: Int
    let total: Int
    let message: String
    let itemEvent: BackupItemEvent?

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
