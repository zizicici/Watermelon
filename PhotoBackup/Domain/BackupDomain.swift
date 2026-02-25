import Foundation
import Photos

struct LocalPhotoResource {
    let asset: PHAsset
    let resource: PHAssetResource
    let assetLocalIdentifier: String
    let resourceLocalIdentifier: String
    let resourceType: String
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
    let completed: Int
    let total: Int
    let message: String

    var fraction: Float {
        guard total > 0 else { return 0 }
        return Float(completed) / Float(total)
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
